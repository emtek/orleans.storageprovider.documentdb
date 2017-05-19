﻿//*********************************************************
//    Copyright (c) Microsoft. All rights reserved.
//    
//    Apache 2.0 License
//    
//    You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//    
//    Unless required by applicable law or agreed to in writing, software 
//    distributed under the License is distributed on an "AS IS" BASIS, 
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
//    implied. See the License for the specific language governing 
//    permissions and limitations under the License.
//
//*********************************************************
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;

namespace Test.Interfaces
{
    [Serializable]
    public class PersonalAttributes
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

    /// <summary>
    /// Orleans grain communication interface IPerson
    /// </summary>
    public interface IPerson : IGrainWithIntegerKey
    {
        Task Register(PersonalAttributes person);
        Task Marry(Guid spouse, string newLastName = null);

        Task<string> GetFirstName();
        Task<string> GetLastName();
        Task<bool> GetIsMarried();
    }
}
